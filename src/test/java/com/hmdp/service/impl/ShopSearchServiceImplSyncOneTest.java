package com.hmdp.service.impl;

import com.hmdp.document.ShopDoc;
import com.hmdp.entity.Shop;
import com.hmdp.repository.ShopDocRepository;
import com.hmdp.service.IShopService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 增量同步的落索引环节（{@code syncShopById}）—— 纯 Mockito。
 *
 * <p>
 * 这里能测、也只能靠测的地方是<strong>「以 MySQL 为准」这个决策</strong>：事件只带 id，
 * 文档内容全部来自回查。它同时兜住了两个真实故障：
 * ① updateById 只更新非空字段，事件里的实体会把没提交的字段写成 null；
 * ② 两条事件乱序/重放时，带快照会写回旧值，带 id 永远取到当前值。
 * ES 本身在跑不起来的机器上测不到，所以这段判断逻辑必须有单测兜着。
 */
@DisplayName("ShopSearchServiceImpl 的按 id 增量同步")
class ShopSearchServiceImplSyncOneTest {

    private static final long SHOP_ID = 7L;

    private final ShopSearchServiceImpl service = new ShopSearchServiceImpl();
    private final IShopService shopService = mock(IShopService.class);
    private final ShopDocRepository shopDocRepository = mock(ShopDocRepository.class);

    @BeforeEach
    void injectCollaborators() {
        ReflectionTestUtils.setField(service, "shopService", shopService);
        ReflectionTestUtils.setField(service, "shopDocRepository", shopDocRepository);
    }

    @Test
    @DisplayName("MySQL 里有这行：按库里的整行写文档，不用事件里的字段")
    void upsertsFromMysqlNotFromTheEvent() {
        Shop stored = new Shop();
        stored.setId(SHOP_ID);
        stored.setName("库里改过的名字");
        stored.setArea("西湖");
        stored.setAvgPrice(120L);
        when(shopService.getById(SHOP_ID)).thenReturn(stored);

        service.syncShopById(SHOP_ID);

        ArgumentCaptor<ShopDoc> doc = ArgumentCaptor.forClass(ShopDoc.class);
        verify(shopDocRepository).save(doc.capture());
        assertEquals(SHOP_ID, doc.getValue().getId());
        assertEquals("库里改过的名字", doc.getValue().getName());
        assertEquals("西湖", doc.getValue().getArea());
        assertEquals(120L, doc.getValue().getAvgPrice());
        verify(shopDocRepository, never()).deleteById(anyLong());
    }

    @Test
    @DisplayName("行已不存在：索引里的文档要跟着删掉（项目此前没有任何删文档路径）")
    void deletesTheDocWhenTheRowIsGone() {
        when(shopService.getById(SHOP_ID)).thenReturn(null);
        when(shopDocRepository.existsById(SHOP_ID)).thenReturn(true);

        service.syncShopById(SHOP_ID);

        verify(shopDocRepository).deleteById(SHOP_ID);
        verify(shopDocRepository, never()).save(any(ShopDoc.class));
    }

    @Test
    @DisplayName("行不存在且索引里也没有：不发那次注定 404 的 DELETE")
    void skipsDeleteWhenTheIndexNeverHadIt() {
        when(shopService.getById(SHOP_ID)).thenReturn(null);
        when(shopDocRepository.existsById(SHOP_ID)).thenReturn(false);

        service.syncShopById(SHOP_ID);

        verify(shopDocRepository, never()).deleteById(anyLong());
    }

    @Test
    @DisplayName("写不进 ES 就往外抛：吞掉异常会把一条失败同步标成已发送")
    void letsEsFailuresPropagate() {
        when(shopService.getById(SHOP_ID)).thenReturn(newShop());
        when(shopDocRepository.save(any(ShopDoc.class)))
                .thenThrow(new RuntimeException("connection refused"));

        assertThrows(RuntimeException.class, () -> service.syncShopById(SHOP_ID));
    }

    private static Shop newShop() {
        Shop shop = new Shop();
        shop.setId(SHOP_ID);
        shop.setName("x");
        return shop;
    }
}
